from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        F.min("rating").over(w_ordered).alias("running_min_rating"),
        F.avg("rating").over(w_ordered).alias("running_avg_rating"),
        F.count("rating").over(w_ordered).alias("running_count_rating"),
        F.sum("rating").over(w_ordered).alias("running_sum_rating"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)
