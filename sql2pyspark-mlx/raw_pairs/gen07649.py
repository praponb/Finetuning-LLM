from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country", "segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.month("signup_date"), "city")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

cleaned = (
    df_customers
    .filter(F.col("credit_limit") > 13900)
    .withColumn("clean_country", F.trim("country"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("DIRECT")))
)

scored = (
    cleaned
    .filter(F.col("signup_date") < "2023-09-28")
    .select(
        "id",
        "name",
        "email",
        "segment",
        "resolved_city",
        F.first("age", ignorenulls=True).over(w_grp).alias("first_known_age"),
        F.first("city", ignorenulls=True).over(w2).alias("first_known_city"),
        F.count("credit_limit").over(w_ordered).alias("signup_date_month_count_credit_limit"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.channel").alias("order_channel"))
)

kept_keys = (
    with_last
    .select("segment")
    .intersect(
        df_customers
        .filter(F.col("country").isin(["US", "SG", "TH"]))
        .select("segment")
    )
)

kept_rows = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("order_channel")
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.sort_array(F.collect_set("order_channel")).alias("order_channel_list"),
    )
    .orderBy(F.col("corporate_rows").desc_nulls_first())
)
