from pyspark.sql import functions as F

avg_revenue = df_store_sales.filter(F.col("units").between(13, 15)).agg(F.avg("revenue")).first()[0]

result = (
    df_store_sales
    .filter(F.col("revenue") > avg_revenue)
    .select("id", "revenue", "units", "sale_date")
    .orderBy(F.col("id").desc(), F.col("units").desc())
)
