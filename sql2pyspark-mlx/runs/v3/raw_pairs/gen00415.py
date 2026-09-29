from pyspark.sql import functions as F

valid_customers = df_customers.filter(F.col("signup_date").between("2024-08-23", "2025-01-01"))

customers_stats = (
    valid_customers
    .groupBy("segment")
    .agg(F.sort_array(F.collect_set("segment")).alias("segment_list"))
)

result = (
    customers_stats
    .select("segment", "segment_list")
    .orderBy("segment")
)
