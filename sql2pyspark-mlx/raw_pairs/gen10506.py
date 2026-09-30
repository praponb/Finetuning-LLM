from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(F.col("budget").isNotNull() & ~F.col("location").isin(["London", "Berlin", "New York"])))
    .select("id", "budget")
)
