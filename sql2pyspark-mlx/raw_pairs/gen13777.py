from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") >= 12)
    .select("city")
    .distinct()
)
