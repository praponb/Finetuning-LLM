from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.round((F.col("rating") / F.sum("rating").over(w)), 3).alias("rating_share"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
    .orderBy(F.col("rating").desc())
)
