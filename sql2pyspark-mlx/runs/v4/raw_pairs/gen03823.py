from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.year("delivered_date").alias("delivered_year"))
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .filter(F.col("dhl_count") < 8)
    .select("delivered_year", "dhl_count", "status_list")
    .orderBy(F.col("delivered_year").desc())
)
