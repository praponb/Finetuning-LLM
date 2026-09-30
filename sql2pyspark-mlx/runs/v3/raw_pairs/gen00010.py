from pyspark.sql import functions as F

result = (
    df_suppliers
    .withColumn("country_city", F.concat("country", F.lit("/"), "city"))
    .withColumn("rating_whole", F.floor("rating"))
)
