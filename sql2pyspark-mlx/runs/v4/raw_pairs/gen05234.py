from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age").desc_nulls_last())
w2 = Window.partitionBy("country").orderBy(F.col("age").desc_nulls_first())
w3 = Window.partitionBy("city", "country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-06-19"))) & F.col("is_active"))
    .withColumn("age_whole", F.ceil("age"))
    .withColumn("segment_group", F.when(F.col("segment").isin(["Corporate", "SMB"]), "Group A").otherwise("Other"))
)

result = (
    filtered
    .select(
        "*",
        F.rank().over(w).alias("age_rank"),
        F.cume_dist().over(w2).alias("age_cume_dist"),
        F.sum("age_whole").over(w3).alias("running_age_whole"),
    )
)
