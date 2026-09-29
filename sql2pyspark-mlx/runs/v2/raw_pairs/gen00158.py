from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") < "2023-12-03")
    .orderBy("sale_date", "id")
    .limit(1)
)
