from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.partitionBy("country")

result = (
    df_suppliers
    .filter(F.col("rating").between(3.4, 3.9))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.max("rating").over(w_ordered).alias("running_max_rating"),
        F.min("rating").over(w_ordered).alias("running_min_rating"),
        F.avg("rating").over(w_ordered).alias("running_avg_rating"),
        F.max("rating").over(w_ordered_2).alias("country_max_rating"),
    )
)
