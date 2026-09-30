from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("YEAR", "delivered_date").alias("year_start"))
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .filter(F.col("dhl_count") >= 1)
    .orderBy(F.col("year_start").desc_nulls_first())
)
