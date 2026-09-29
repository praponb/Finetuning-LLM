from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "IN_TRANSIT"]))
    .groupBy(F.year("shipped_date").alias("shipped_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0))) / F.count("*")), 1).alias("pct_dhl"),
    )
    .filter(F.col("pct_dhl") >= 24)
    .orderBy(F.col("shipped_year").desc())
)
