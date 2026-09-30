from pyspark.sql import functions as F

result = (
    df_customers
    .withColumn("signup_date_plus_months", F.add_months("signup_date", 12))
    .withColumn("city_upper", F.upper("city"))
    .orderBy(F.col("credit_limit").desc(), "segment", "id")
    .limit(3)
)
