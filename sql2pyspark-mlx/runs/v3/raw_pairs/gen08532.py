from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.sum("rating").over(w).alias("running_rating"),
    )
)
