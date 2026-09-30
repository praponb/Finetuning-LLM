from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("adjusted_rating", F.round((F.col("rating") * 0.85), 2))
    .withColumn("city_upper", F.upper("city"))
    .orderBy(F.col("id").desc(), F.col("country").desc())
    .limit(5)
)
