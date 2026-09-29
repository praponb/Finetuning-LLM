from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.col("carrier") != "Flash")

shipped_year_summary = (
    filtered_shipments
    .groupBy(F.year("shipped_date").alias("shipped_year"), "carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = (
    shipped_year_summary
    .select("shipped_year", "carrier", "status_values")
    .orderBy("status_values", F.col("carrier").desc(), "shipped_year")
    .limit(50)
)
