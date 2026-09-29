from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "Berlin") & (F.year("opened_date") >= 2023))
    .withColumn("is_south", F.when(F.col("region") == "South", 1).otherwise(0))
    .withColumn("store_name_filled", F.coalesce("store_name", F.lit("unassigned")))
    .orderBy(F.col("store_name_filled").desc())
)
