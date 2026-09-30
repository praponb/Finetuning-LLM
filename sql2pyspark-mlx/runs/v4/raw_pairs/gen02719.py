from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last())
w_grp = Window.partitionBy("city", "country").orderBy("rating", "id")

scored = (
    df_suppliers
    .select(
        "*",
        F.avg("rating").over(w).alias("rating_avg_2"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.dense_rank().over(w3).alias("rating_dense_rank"),
        F.lag("rating", 2).over(w_grp).alias("prev_rating"),
    )
)

result = (
    scored
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating_avg_2",
        "rating_dense_rank",
        "prev_rating",
        F.when(F.col("supplier_name").rlike("^(Global Supply|Pacific Wholesale)$"), "PRIORITY").otherwise("EXTENDED").alias("supplier_name_zone"),
    )
    .orderBy(F.col("prev_rating").desc())
)
