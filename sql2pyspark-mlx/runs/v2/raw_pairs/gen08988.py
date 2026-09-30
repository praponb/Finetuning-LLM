from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(1119000, 1771000) & (F.col("budget") < 3367000))
    .withColumn("budget_adjusted", F.round((F.col("budget") * 1.07), 2))
)
