from pyspark.sql import functions as F

avg_units = df_store_sales.filter(F.year("sale_date") == 2023).agg(F.avg("units")).first()[0]

result = (
    df_store_sales
    .filter(F.col("units") > avg_units)
    .select("id", "units", "product_id")
    .orderBy(F.col("product_id").desc())
)
