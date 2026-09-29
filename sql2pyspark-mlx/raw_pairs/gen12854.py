from pyspark.sql import functions as F

result = (
    df_customers
    .select("name")
    .union(df_shipments.select("carrier"))
    .distinct()
    .orderBy("name")
)
