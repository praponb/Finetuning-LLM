from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Sales", 1).otherwise(0)).alias("sales_count"),
    )
    .filter(F.col("sales_count") >= 62)
    .select("location", "sales_count")
)
