from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_code_len_by_sku_code_len = Window.partitionBy("sku_code_len").orderBy(F.col("sku_code_len").desc())
w_sku_code_len_by_sku_code_len_2 = Window.partitionBy("sku_code_len").orderBy(F.col("sku_code_len").desc(), "id").rowsBetween(-2, Window.currentRow)
w_sku_code_len_by_sku_code_len_3 = Window.partitionBy("sku_code_len").orderBy(F.col("sku_code_len").desc(), "id")
w_sku_code_len_is_active_by_sk = Window.partitionBy("sku_code_len", "is_active").orderBy("sku_code_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_code_len_is_active_by_sk_2 = Window.partitionBy("sku_code_len", "is_active").orderBy("sku_code_len")
w_sku_code_len = Window.partitionBy("sku_code_len")

recent_skus = (
    df_skus
    .filter((F.col("weight_kg") >= 3.7) | (F.col("unit_cost") <= 320.4))
    .select(
        "id",
        "sku_code",
        "category",
        "is_active",
        F.length(F.trim(F.col("sku_code"))).alias("sku_code_len"),
    )
)

ranked = (
    recent_skus
    .select(
        "*",
        F.dense_rank().over(w_sku_code_len_by_sku_code_len).alias("sku_code_len_dense_rank"),
        F.max("sku_code_len").over(w_sku_code_len_by_sku_code_len_2).alias("sku_code_len_max_3"),
        F.round(F.cume_dist().over(w_sku_code_len_by_sku_code_len_3), 3).alias("sku_code_len_cume_dist"),
        F.last("sku_code_len", ignorenulls=True).over(w_sku_code_len_is_active_by_sk).alias("last_known_sku_code_len"),
        F.round(F.percent_rank().over(w_sku_code_len_is_active_by_sk_2), 3).alias("sku_code_len_percent_rank"),
        F.max("sku_code_len").over(w_sku_code_len).alias("sku_code_len_max_sku_code_len"),
    )
)

tagged = (
    ranked
    .select(
        "id",
        "sku_code",
        "is_active",
        "sku_code_len",
        "sku_code_len_dense_rank",
        "sku_code_len_max_3",
        "sku_code_len_max_sku_code_len",
        F.round(F.col("sku_code_len") / F.nullif(F.col("last_known_sku_code_len"), F.lit(0)), 3).alias("sku_code_len_per_last_known_sku_code_len"),
        F.upper(F.trim(F.col("category"))).alias("category_clean"),
    )
)

result = (
    tagged
    .filter(F.col("sku_code_len_per_last_known_sku_code_len") <= 6.5)
    .select(
        "id",
        "is_active",
        "sku_code_len",
        "sku_code_len_per_last_known_sku_code_len",
    )
    .orderBy(F.col("id").desc())
)
