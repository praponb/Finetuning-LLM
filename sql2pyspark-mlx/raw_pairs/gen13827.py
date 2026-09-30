from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") > 38)
    .select("id", "sale_date", "store_id")
    .orderBy("store_id")
)
