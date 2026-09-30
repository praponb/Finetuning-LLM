from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2675000, 3063000) | F.col("location").isin(["Tokyo", "London", "Bangkok"]))
    .select("location")
)
