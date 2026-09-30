from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") > 9600)
    .select("city")
    .unionByName(df_stores.filter(F.col("store_name").like("%Tow%")).select("city"))
    .distinct()
    .orderBy("city")
)
