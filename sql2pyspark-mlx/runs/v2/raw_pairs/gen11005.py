from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.upper(F.trim("name")) == "FRANK MILLER")
    .orderBy("city")
)
