from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("country")
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("discount").alias("sum_order_discount"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("tag"),
    )
)

valid = (
    exploded
    .filter((F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 12:00:00"))) & ~F.col("tag").isin(["VIP", "WELCOME5"]))
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
    .withColumn("resolved_tag", F.coalesce("tag", F.lit("DIRECT")))
)

enriched = (
    valid
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.age"), F.col("p.country"))
)

enriched_carts = (
    enriched
    .filter(F.col("age") < 49)
    .select(
        "*",
        F.count("days_since_created_at").over(w).alias("days_since_created_at_count_2h"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
    )
)

result = (
    enriched_carts
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "created_at",
        "tag",
        "age",
        "country",
        "days_since_created_at_count_2h",
        "age_share",
        F.when(F.col("country").rlike("^(TH|US)$"), "CORE").otherwise("EXTENDED").alias("country_zone"),
        F.col("sum_order_discount"),
    )
    .orderBy(F.col("tag").desc_nulls_last())
)
