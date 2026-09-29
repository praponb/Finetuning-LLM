from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"))
)

result = (
    with_latest
    .withColumn("quarter_start", F.date_trunc("quarter", "opened_date"))
    .groupingSets(
        [["region", "quarter_start"], ["region"], []],
        "region",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count_2"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_rows"),
    )
    .filter(F.col("east_count_2") > 1)
    .orderBy(F.col("east_count_2").desc_nulls_first())
)
