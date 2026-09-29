from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.upper(F.trim("supplier_name")) == "LION CITY IMPORTS") | F.col("city").isin(["New York", "Berlin", "Bangkok"]))
    .select("city", "rating")
    .orderBy("rating")
)
