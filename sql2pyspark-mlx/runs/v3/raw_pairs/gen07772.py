from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") > 2.9)
    .select(
        "id",
        "country",
        "rating",
        F.round((F.col("rating") / F.sum("rating").over(w)), 3).alias("rating_share"),
        F.rank().over(w2).alias("rating_rank"),
    )
)
