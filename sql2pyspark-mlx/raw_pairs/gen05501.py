from pyspark.sql import functions as F

result = (
    df_customers
    .select("id", "country", "signup_date")
    .orderBy("country")
)
