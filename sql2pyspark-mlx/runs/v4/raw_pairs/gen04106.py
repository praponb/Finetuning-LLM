from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("units") > 17)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)

result = (
    valid
    .withColumn("quarter_start", F.date_trunc("quarter", "sale_date"))
    .groupingSets(
        [["quarter_start", "store_id"], ["quarter_start"], []],
        "quarter_start",
        "store_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.percentile_approx("revenue_per_unit", 0.9).alias("revenue_per_unit_p90"),
        F.max("units").alias("max_units"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
)
