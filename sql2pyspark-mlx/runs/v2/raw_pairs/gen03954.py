from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.length("supplier_name") < 12) & ((F.col("rating") > 2.3) | (F.col("country") != "JP")))
    .orderBy("rating")
)
