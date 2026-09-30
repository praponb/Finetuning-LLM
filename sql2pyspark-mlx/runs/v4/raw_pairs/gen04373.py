from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~((F.col("rating") < 3.5) & (F.col("country").isin(["SG", "DE", "US"]) | F.col("country").isin(["DE", "US"]))))
    .withColumn("supplier_name_upper", F.upper("supplier_name"))
    .withColumn("rating_rounded", F.round("rating", 2))
)
