from pyspark.sql import functions as F

max_units = df_store_sales.agg(F.max("units")).first()[0]

result = (
    df_store_sales
    .filter(F.col("units") >= max_units)
    .select("id", "units", "sale_date", "product_id")
    .orderBy(F.col("id").desc(), F.col("units").desc())
)
