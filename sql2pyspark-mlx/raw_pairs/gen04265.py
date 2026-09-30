from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .pivot("dept_name", ["Sales", "HR"])
    .agg(F.max("budget").alias("total"), F.count(F.lit(1)).alias("item_count"))
)
