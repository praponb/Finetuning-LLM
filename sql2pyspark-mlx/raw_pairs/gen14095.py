from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country")
w2 = Window.partitionBy("country")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.round((F.col("rating") / F.sum("rating").over(w)), 3).alias("rating_share"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 3).alias("rating_share_2"),
    )
    .orderBy("rating_share_2")
)
