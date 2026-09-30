from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_suppliers
    .filter(F.col("country").isin(["UK", "JP", "SG"]))
    .select(
        "id",
        "country",
        "rating",
        F.max("rating").over(w_recent).alias("last3_max_rating"),
        F.count("rating").over(w_recent).alias("last3_count_rating"),
        F.min("rating").over(w_recent).alias("last3_min_rating"),
    )
)
