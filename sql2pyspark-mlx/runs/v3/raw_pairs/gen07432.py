from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") <= "2023-06-27") & (F.col("age") < 43) & F.col("name").like("%Ivy%"))
    .withColumn("signup_date_plus_37d", F.date_add("signup_date", 37))
    .withColumn("email_user", F.split(F.lower("email"), "@").getItem(0))
    .orderBy("signup_date_plus_37d")
)
