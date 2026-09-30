from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.col("status") != "DELIVERED")

shipped_quarter_summary = (
    recent_shipments
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(F.sort_array(F.collect_set("carrier")).alias("carrier_list"))
)

result = (
    shipped_quarter_summary
    .select("shipped_quarter", "carrier_list")
    .orderBy(F.col("shipped_quarter").desc())
)
