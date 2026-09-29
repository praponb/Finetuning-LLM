from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull() & F.col("country").isin(["UK", "JP"]))
    .withColumn("country_upper", F.upper("country"))
    .orderBy(F.col("rating").desc(), "id")
    .limit(5)
)
