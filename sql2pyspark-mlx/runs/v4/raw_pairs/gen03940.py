from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower(F.trim("store_name")) == "mall central")
    .select("opened_date")
    .intersect(df_customers.select("signup_date"))
)
