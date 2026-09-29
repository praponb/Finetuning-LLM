from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
    .orderBy("rating", F.col("city").desc())
)
