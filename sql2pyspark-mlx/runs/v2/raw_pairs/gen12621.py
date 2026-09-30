from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") > 14)
    .select("credit_limit", "city", "name")
)
