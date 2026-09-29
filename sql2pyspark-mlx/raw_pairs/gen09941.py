from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.round((F.col("rating") / F.sum("rating").over(w)), 3).alias("rating_share"),
        F.first("city").over(w2).alias("first_city"),
    )
    .orderBy("city")
)
