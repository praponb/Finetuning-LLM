from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-5, Window.currentRow)
w_recent = Window.partitionBy("country")
w_part = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_5"),
        F.min("rating").over(w_recent).alias("country_min_rating"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)
