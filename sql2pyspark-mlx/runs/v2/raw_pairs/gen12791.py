from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.lower(F.trim("product_name")) == "coffee maker") & F.col("supplier_id").isNotNull())
    .withColumn("product_name_upper", F.upper("product_name"))
    .withColumn("is_stark", F.when(F.col("brand") == "Stark", 1).otherwise(0))
)
