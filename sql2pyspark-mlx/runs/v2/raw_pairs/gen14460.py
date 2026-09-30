from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(~((F.col("dept_name") == "Engineering") | (F.col("budget") < 2223000)))
    .withColumn("location_filled", F.coalesce("location", F.lit("N/A")))
)

agg = (
    filtered_departments
    .groupBy("dept_name")
    .agg(
        F.sum(F.when(F.col("dept_name") == "Finance", 1).otherwise(0)).alias("finance_count"),
    )
)

result = agg.orderBy("dept_name", "finance_count")
