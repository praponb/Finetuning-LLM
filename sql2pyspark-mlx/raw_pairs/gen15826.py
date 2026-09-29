from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value_2")

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

lines = flattened.select("created_at", F.explode_outer("coupon_codes").alias("value_2"))

cleaned = (
    lines
    .filter((F.col("created_at") > F.to_date(F.lit("2025-03-02"))) & F.col("value_2").isNotNull())
    .select(
        "*",
        F.when(F.col("value_2") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
        F.trim("value_2").alias("clean_value_2"),
        F.coalesce("value_2", F.lit("N/A")).alias("resolved_value_2"),
    )
)

enriched = cleaned.withColumn("value_2_sum_is_welcome5", F.sum("is_welcome5").over(w))

result = (
    enriched
    .select(
        "created_at",
        "value_2",
        "is_welcome5",
        "clean_value_2",
        "value_2_sum_is_welcome5",
    )
    .orderBy(F.col("created_at").asc_nulls_last())
)
