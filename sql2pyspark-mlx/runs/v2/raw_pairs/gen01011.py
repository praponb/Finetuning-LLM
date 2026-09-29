from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter((F.col("shipped_date") <= "2023-10-01") & ((F.col("shipped_date") > "2023-07-01") | F.col("shipped_date").between("2023-08-01", "2024-01-01")))
)

shipments_stats = (
    filtered_shipments
    .groupBy("carrier")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    shipments_stats
    .select("carrier", "total_cost")
    .orderBy(F.col("total_cost").desc(), F.col("carrier").desc())
)
