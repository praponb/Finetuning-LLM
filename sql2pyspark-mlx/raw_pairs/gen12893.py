from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.max("rating").over(w).alias("running_max_rating"),
        F.count("rating").over(w).alias("running_count_rating"),
        F.min("rating").over(w).alias("running_min_rating"),
    )
)
