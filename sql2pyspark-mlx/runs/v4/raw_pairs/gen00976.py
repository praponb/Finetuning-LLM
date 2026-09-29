from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.lower(F.trim("supplier_name")) == "pacific wholesale")
    .select(
        "id",
        "city",
        "rating",
        F.round((F.col("rating") / F.sum("rating").over(w)), 3).alias("rating_share"),
        F.lead("rating", 1).over(w2).alias("next_rating"),
    )
    .orderBy(F.col("rating").desc(), "city")
)
