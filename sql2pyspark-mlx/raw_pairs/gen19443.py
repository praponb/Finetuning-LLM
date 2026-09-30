from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "store_id", F.least("revenue", "units").alias("least_revenue_units"))
    .orderBy("store_id", "id")
    .limit(20)
)
