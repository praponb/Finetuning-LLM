from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("country", "city")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("city").orderBy("rating", "id")

metrics = (
    df_suppliers
    .select(
        "*",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share_2"),
        F.row_number().over(w4).alias("seq_no"),
        F.ntile(3).over(w).alias("rating_bucket"),
    )
)

result = (
    metrics
    .cube(
        F.when(F.col("rating_share_2") >= 0.61, "PREMIUM").otherwise("STANDARD").alias("rating_share_2_tier"),
        "country",
    )
    .agg(
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.round(F.sum("seq_no") / F.nullif(F.count(F.when(F.col("country") == "JP", 1)), F.lit(0)), 2).alias("seq_no_per_jp"),
    )
)
