from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("shipped_date") <= "2025-03-01") & F.month("delivered_date").isin([7, 10, 12])) | (F.col("order_id") == 16))
    .groupBy("status", "carrier")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0))) / F.count("*")), 1).alias("pct_dhl"),
    )
    .orderBy(F.col("pct_dhl").desc(), F.col("status").desc())
)
