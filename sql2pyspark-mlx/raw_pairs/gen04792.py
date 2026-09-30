from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") < 31)
    .select("country")
    .unionByName(df_suppliers.select("country"))
    .orderBy("country")
)
