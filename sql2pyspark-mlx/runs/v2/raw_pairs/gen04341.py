from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([4, 8, 9]) & (F.col("city") == "New York") & (F.col("signup_date") >= "2023-04-02"))
    .orderBy("country")
)
