from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "store_id", "sale_date")
    .orderBy("sale_date", "id")
    .limit(1)
)
