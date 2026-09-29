from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.year("sale_date") >= 2025) & F.col("revenue").between(1700, 2000))
)
