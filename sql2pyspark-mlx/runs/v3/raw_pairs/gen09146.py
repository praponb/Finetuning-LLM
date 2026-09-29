from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_year")

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2023-12-01", "2025-08-01"))
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("*").alias("cnt"),
    )
    .withColumn("cumulative_revenue", F.sum("total_revenue").over(w))
    .orderBy(F.col("sale_year").desc())
)
