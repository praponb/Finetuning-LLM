from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.round(F.sum("budget") / F.nullif(F.count(F.when(F.col("dept_name") == "Sales", 1)), F.lit(0)), 2).alias("budget_per_sales"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
)
