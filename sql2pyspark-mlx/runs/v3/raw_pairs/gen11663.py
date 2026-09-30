from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("sale_date").alias("event_date"))
    .unionByName(
        df_employees
        .filter(F.col("dept_id").isin([7, 12]))
        .select(F.col("hire_date").alias("event_date"))
    )
    .distinct()
    .orderBy("event_date")
)
