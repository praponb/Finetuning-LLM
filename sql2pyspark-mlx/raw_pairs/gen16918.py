from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").isNull() & F.col("delivered_date").between("2024-08-01", "2024-12-21"))
    .groupBy(F.year("shipped_date").alias("shipped_year"), "order_id")
    .agg(
        F.min("cost").alias("min_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0))) / F.count("*")), 1).alias("pct_dhl"),
        F.sum("cost").alias("total_cost"),
    )
    .filter(F.col("total_cost") > 460)
    .drop("total_cost")
    .orderBy(F.col("pct_dhl").desc(), "shipped_year", "order_id")
    .limit(3)
)
