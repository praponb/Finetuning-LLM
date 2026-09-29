from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .unionByName(df_suppliers.select("city"))
    .orderBy("city")
)
