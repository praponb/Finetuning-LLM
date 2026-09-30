from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(F.sum("age").alias("total_age"))
    .withColumn("pct_of_total", F.round(((F.col("total_age") * 100) / F.sum("total_age").over(w)), 2))
    .orderBy(F.col("signup_year").desc())
)
